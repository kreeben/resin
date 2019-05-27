using System;
using Sir.Store;

namespace Sir.StringCompare
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args[0] == "-b")
            {
                var root = new VectorNode();

                while (true)
                {
                    Console.WriteLine("enter word:");

                    var command = Console.ReadLine();

                    if (string.IsNullOrWhiteSpace(command))
                    {
                        break;
                    }

                    VectorNodeWriter.Add(root, new VectorNode(command.ToVector(0, command.Length)), Similarity.Term);
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

                    var hit = VectorNodeReader.ClosestMatch(root, command.ToVector(0, command.Length), Similarity.Term.foldAngle);

                    Console.WriteLine($"{hit.Score} {hit.Node}");
                }
            }
            else
            {
                var doc1 = new VectorNode(args[0]);
                var doc2 = new VectorNode(args[1]);
                var angle = doc1.Vector.CosAngle(doc2.Vector);

                Console.WriteLine($"similarity: {angle}");
            }
        }
    }
}