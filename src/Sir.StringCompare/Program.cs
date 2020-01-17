using Sir.Search;
using Sir.VectorSpace;
using System;
using System.Linq;

namespace Sir.StringCompare
{
    class Program
    {
        static void Main(string[] args)
        {
            var model = new BocModel();

            if (args[0] == "-b" || args[0] == "--build-graph")
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

                    var node = new VectorNode(model.Tokenize(command.ToCharArray()).First());
                    GraphBuilder.TryMerge(root, node, model, model.FoldAngle, model.IdenticalAngle, out _);
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
            else
            {
                var doc1 = new VectorNode(model.Tokenize(args[0].ToCharArray()).First());
                var doc2 = new VectorNode(model.Tokenize(args[1].ToCharArray()).First());
                var angle = model.CosAngle(doc1.Vector, doc2.Vector);
                Console.WriteLine($"similarity: {angle}");
            }
        }
    }
}