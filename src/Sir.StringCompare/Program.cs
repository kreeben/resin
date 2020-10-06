using Sir.Search;
using Sir.VectorSpace;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Sir.StringCompare
{
    class Program
    {
        static void Main(string[] args)
        {
            var model = new BocModel();

            if (args[0] == "--build-graph")
            {
                RunInteractiveGraphBuilder(model);
            }
            else
            {
                CompareBaseless(args[0], args[1], model);
                Compare(args[0], args[1], model);
            }
        }

        private static void Compare(string first, string second, IStringModel model)
        {
            var baseVectorComponents = new List<float>(model.VectorWidth);
            var baseVectors = new List<IVector>();

            for (int i = 0; i < model.VectorWidth; i++)
            {
                baseVectorComponents.Add(i == 0 ? 1 : Convert.ToSingle(Math.Log10(i)));

                var bvecs = new List<float>(model.VectorWidth);

                for (int y = 0; y< model.VectorWidth; y++)
                {
                    float value;

                    if (y == i)
                    {
                        value = 1;
                    }
                    else
                    {
                        value = 0;
                    }

                    bvecs.Add(value);
                }

                baseVectors.Add(new IndexedVector(bvecs, model.VectorWidth));
            }

            var bvector = new IndexedVector(baseVectorComponents, model.VectorWidth);

            var doc1 = new VectorNode(model.Tokenize(first.ToCharArray()).First());
            var doc2 = new VectorNode(model.Tokenize(second.ToCharArray()).First());
            var angles1 = new List<float>();
            var angles2 = new List<float>();

            foreach (var bvec in baseVectors)
            {
                angles1.Add(Convert.ToSingle(model.CosAngle(doc1.Vector, bvec)));
                angles2.Add(Convert.ToSingle(model.CosAngle(doc2.Vector, bvec)));
            }

            var docVector1 = new IndexedVector(angles1, model.VectorWidth);
            var docVector2 = new IndexedVector(angles2, model.VectorWidth);

            var angle = model.CosAngle(docVector1, docVector2);
            var angle1 = model.CosAngle(docVector1, bvector);
            var angle2 = model.CosAngle(docVector2, bvector);

            Console.WriteLine($"similarity: {angle}");
            Console.WriteLine($"bvector similarity 1: {angle1}");
            Console.WriteLine($"bvector similarity 2: {angle2}");
            Console.WriteLine($"base vector similarity: {Math.Min(angle1, angle2) / Math.Max(angle1, angle2)}");
        }

        private static void CompareBaseless(string first, string second, IStringModel model)
        {
            var doc1 = new VectorNode(model.Tokenize(first.ToCharArray()).First());
            var doc2 = new VectorNode(model.Tokenize(second.ToCharArray()).First());

            var angle = model.CosAngle(doc1.Vector, doc2.Vector);

            Console.WriteLine($"similarity (baseless): {angle}");
        }

        private static void RunInteractiveGraphBuilder(IStringModel model)
        {
            var root = new VectorNode();

            while (true)
            {
                Console.WriteLine("enter text:");

                var command = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(command))
                {
                    break;
                }

                var node = new VectorNode(model.Tokenize(command.ToCharArray()).First());

                GraphBuilder.MergeOrAdd(root, node, model, model.FoldAngle, model.IdenticalAngle);
            }

            Console.WriteLine(root.Visualize());

            while (true)
            {
                Console.WriteLine("enter query:");

                var command = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(command))
                {
                    break;
                }

                var hit = PathFinder.ClosestMatch(root, model.Tokenize(command.ToCharArray()).First(), model);

                Console.WriteLine($"{hit.Score} {hit.Node}");
            }
        }
    }
}