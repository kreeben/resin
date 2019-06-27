﻿using RocksDbSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;

namespace Sir.Store
{
    public static class GraphBuilder
    {
        public static bool Add(VectorNode root, VectorNode node, IStringModel model)
        {
            var cursor = root;

            while (cursor != null)
            {
                var angle = cursor.Vector.Count > 0 ? model.CosAngle(node.Vector, cursor.Vector) : 0;

                if (angle >= model.IdenticalAngle)
                {
                    lock (cursor.Sync)
                    {
                        Merge(cursor, node);

                        return false;
                    }
                }
                else if (angle > model.FoldAngle)
                {
                    if (cursor.Left == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Left == null)
                            {
                                node.AngleWhenAdded = angle;
                                cursor.Left = node;

                                return true;
                            }
                            else
                            {
                                cursor = cursor.Left;
                            }
                        }
                    }
                    else
                    {
                        cursor = cursor.Left;
                    }
                }
                else
                {
                    if (cursor.Right == null)
                    {
                        lock (cursor.Sync)
                        {
                            if (cursor.Right == null)
                            {
                                node.AngleWhenAdded = angle;
                                cursor.Right = node;

                                return true;
                            }
                            else
                            {
                                cursor = cursor.Right;
                            }
                        }
                    }
                    else
                    {
                        cursor = cursor.Right;
                    }
                }
            }

            return false;
        }

        public static void MergePostings(VectorNode target, VectorNode node)
        {
            if (target.PostingsOffsets == null)
            {
                target.PostingsOffsets = new List<long> { target.PostingsOffset };
            }

            if (node.PostingsOffsets == null)
            {
                target.PostingsOffsets.Add(node.PostingsOffset);
            }
            else
            {
                ((List<long>)target.PostingsOffsets).AddRange(node.PostingsOffsets);
            }
        }

        public static void Merge(VectorNode target, VectorNode node)
        {
            MergeDocIds(target, node);
            MergePostings(target, node);
            
        }

        public static void MergeDocIds(VectorNode target, VectorNode node)
        {
            if (target.DocIds == null || node.DocIds == null)
            {
                return;
            }

            foreach (var docId in node.DocIds)
            {
                target.DocIds.Add(docId);
            }
        }

        public static Vector Compress(VectorNode root)
        {
            var vector = new Vector(new int[0]);

            foreach (var node in PathFinder.All(root))
            {
                vector = vector.Add(node.Vector);
            }

            return vector;
        }

        public static void SerializeNode(VectorNode node, Stream stream)
        {
            long terminator = 1;

            if (node.Left == null && node.Right == null) // there are no children
            {
                terminator = 3;
            }
            else if (node.Left == null) // there is a right but no left
            {
                terminator = 2;
            }
            else if (node.Right == null) // there is a left but no right
            {
                terminator = 1;
            }
            else // there is a left and a right
            {
                terminator = 0;
            }

            Span<long> span = stackalloc long[5];

            span[0] = node.VectorOffset;
            span[1] = node.PostingsOffset;
            span[2] = node.Vector.Count;
            span[3] = node.Weight;
            span[4] = terminator;

            stream.Write(MemoryMarshal.Cast<long, byte>(span));
        }

        public static void SerializeNode(VectorNode node, RocksDb db, ColumnFamilyHandle cf)
        {
            long terminator = 1;

            if (node.Left == null && node.Right == null) // there are no children
            {
                terminator = 3;
            }
            else if (node.Left == null) // there is a right but no left
            {
                terminator = 2;
            }
            else if (node.Right == null) // there is a left but no right
            {
                terminator = 1;
            }
            else // there is a left and a right
            {
                terminator = 0;
            }

            Span<long> span = stackalloc long[5];

            span[0] = node.VectorOffset;
            span[1] = node.PostingsOffset;
            span[2] = node.Vector.Count;
            span[3] = node.Weight;
            span[4] = terminator;
            
            db.Put(
                MemoryMarshal.Cast<long, byte>(span.Slice(0, sizeof(long))).ToArray(),
                MemoryMarshal.Cast<long, byte>(span).ToArray(), cf: cf);
        }

        public static (long offset, long length) SerializeTree(
            VectorNode node, 
            RocksDb db, 
            ColumnFamilyHandle cf, 
            Stream vectorStream, 
            Stream postingsStream, 
            IStringModel tokenizer)
        {
            var stack = new Stack<VectorNode>();

            if (node.Vector.Count == 0)
            {
                node = node.Right;
            }

            long offset = -1;
            long count = 0;

            while (node != null)
            {
                SerializePostings(node, postingsStream);
                node.VectorOffset = tokenizer.SerializeVector(node.Vector, vectorStream);
                SerializeNode(node, db, cf);

                if (offset == -1)
                {
                    offset = node.VectorOffset;
                }

                count++;

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null && stack.Count > 0)
                {
                    node = stack.Pop();
                }
            }

            return (offset, count);
        }

        public static (long offset, long length) SerializeTree(
            VectorNode node, 
            Stream indexStream, 
            Stream vectorStream, 
            Stream postingsStream, 
            IStringModel tokenizer)
        {
            var stack = new Stack<VectorNode>();
            var offset = indexStream.Position;

            if (node.Vector.Count == 0)
            {
                node = node.Right;
            }

            while (node != null)
            {
                SerializePostings(node, postingsStream);
                node.VectorOffset = tokenizer.SerializeVector(node.Vector, vectorStream);
                SerializeNode(node, indexStream);

                if (node.Right != null)
                {
                    stack.Push(node.Right);
                }

                node = node.Left;

                if (node == null && stack.Count > 0)
                {
                    node = stack.Pop();
                }
            }

            var length = indexStream.Position - offset;

            return (offset, length);
        }

        public static void SerializePostings(VectorNode node, Stream postingsStream)
        {
            var offset = postingsStream.Position;

            postingsStream.Write(node.DocIds.ToStreamWithHeader(node.DocIds.Count));

            node.PostingsOffset = offset;
        }

        public static VectorNode DeserializeNode(byte[] nodeBuffer, Stream vectorStream, IStringModel tokenizer)
        {
            // Deserialize node
            var vecOffset = BitConverter.ToInt64(nodeBuffer, 0);
            var postingsOffset = BitConverter.ToInt64(nodeBuffer, sizeof(long));
            var vectorCount = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long));
            var weight = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long) + sizeof(long));
            var terminator = BitConverter.ToInt64(nodeBuffer, sizeof(long) + sizeof(long) + sizeof(long) + sizeof(long));

            return DeserializeNode(vecOffset, postingsOffset, vectorCount, weight, terminator, vectorStream, tokenizer);
        }

        public static VectorNode DeserializeNode(
            long vecOffset,
            long postingsOffset,
            long componentCount,
            long weight,
            long terminator,
            Stream vectorStream,
            IStringModel tokenizer)
        {
            var vector = tokenizer.DeserializeVector(vecOffset, (int)componentCount, vectorStream);
            var node = new VectorNode(postingsOffset, vecOffset, terminator, weight, componentCount, vector);

            return node;
        }

        public static void DeserializeUnorderedFile(
            Stream indexStream,
            Stream vectorStream,
            VectorNode root,
            float identicalAngle, 
            float foldAngle,
            IStringModel model)
        {
            var buf = new byte[VectorNode.BlockSize];
            int read = indexStream.Read(buf);

            while (read == VectorNode.BlockSize)
            {
                var node = DeserializeNode(buf, vectorStream, model);

                if (node.VectorOffset > -1)
                    GraphBuilder.Add(root, node, model);

                read = indexStream.Read(buf);
            }
        }

        public static void DeserializeTree(
            Stream indexStream,
            Stream vectorStream,
            long indexLength,
            VectorNode root,
            (float identicalAngle, float foldAngle) similarity,
            IStringModel model)
        {
            int read = 0;
            var buf = new byte[VectorNode.BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, model);

                if (node.VectorOffset > -1)
                    GraphBuilder.Add(root, node, model);

                read += VectorNode.BlockSize;
            }
        }

        public static VectorNode DeserializeTree(Stream indexStream, Stream vectorStream, long indexLength, IStringModel tokenizer)
        {
            VectorNode root = new VectorNode();
            VectorNode cursor = root;
            var tail = new Stack<VectorNode>();
            int read = 0;
            var buf = new byte[VectorNode.BlockSize];

            while (read < indexLength)
            {
                indexStream.Read(buf);

                var node = DeserializeNode(buf, vectorStream, tokenizer);

                if (node.Terminator == 0) // there is both a left and a right child
                {
                    cursor.Left = node;
                    tail.Push(cursor);
                }
                else if (node.Terminator == 1) // there is a left but no right child
                {
                    cursor.Left = node;
                }
                else if (node.Terminator == 2) // there is a right but no left child
                {
                    cursor.Right = node;
                }
                else // there are no children
                {
                    if (tail.Count > 0)
                    {
                        tail.Pop().Right = node;
                    }
                }

                cursor = node;
                read += VectorNode.BlockSize;
            }

            var right = root.Right;

            right.DetachFromAncestor();

            return right;
        }
    }
}
