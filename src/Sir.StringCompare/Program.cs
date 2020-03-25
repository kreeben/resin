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

            if (args[0] == "--build-graph")
            {
                RunInteractiveGraphBuilder(model);
            }
            else
            {
                Compare(args[0], args[1], model);
            }
        }

        private static void Compare(string first, string second, IStringModel model)
        {
            var doc1 = new VectorNode(model.Tokenize(first.ToCharArray()).First());
            var doc2 = new VectorNode(model.Tokenize(second.ToCharArray()).First());

            var angle = model.CosAngle(doc1.Vector, doc2.Vector);

            Console.WriteLine($"similarity: {angle}");
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