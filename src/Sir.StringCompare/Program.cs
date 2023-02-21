using Sir.IO;
using Sir.Strings;
using System;
using System.Linq;

namespace Sir.StringCompare
{
    class Program
    {
        static void Main(string[] args)
        {
            var model = new BagOfCharsModel();

            if (args[0] == "--build-graph")
            {
                RunInteractiveGraphBuilder(model);
            }
            else
            {
                Similarity(args[0], args[1], model);
                //CompareToBaseVector(args[0], args[1], model);
            }
        }

        //private static void CompareToBaseVector(string first, string second, IModel<string> model)
        //{
        //    var baseVectorStorage = new float[model.NumOfDimensions];

        //    for (int i = 0; i < baseVectorStorage.Length; i++)
        //    {
        //        baseVectorStorage[i] = (float)i + 1;
        //    }

        //    var baseVector = new SerializableVector(baseVectorStorage);
        //    var firstVector = model.CreateEmbedding(first, false).First();
        //    var secondVector = model.CreateEmbedding(second, false).First();
        //    var angle1 = model.CosAngle(baseVector, firstVector);
        //    var angle2 = model.CosAngle(baseVector, secondVector);

        //    Console.WriteLine($"first angle to base vector: {angle1}");
        //    Console.WriteLine($"second angle to base vector: {angle2}");
        //    Console.WriteLine($"base vector similarity: {Math.Min(angle1, angle2) / Math.Max(angle1, angle2)}");
        //}

        private static void Similarity(string first, string second, IModel<string> model)
        {
            var vec1 = model.CreateEmbedding(first, false).First();
            var vec2 = model.CreateEmbedding(second, false).First();
            var angle = model.CosAngle(vec1, vec2);

            Console.WriteLine($"similarity: {angle}");
        }

        private static void RunInteractiveGraphBuilder(IModel<string> model)
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

                var node = new VectorNode(model.CreateEmbedding(command, false).First());

                root.AddOrAppend(node, model);
            }

            Console.WriteLine(PathFinder.Visualize(root));

            while (true)
            {
                Console.WriteLine("enter query:");

                var command = Console.ReadLine();

                if (string.IsNullOrWhiteSpace(command))
                {
                    break;
                }

                var hit = PathFinder.ClosestMatch(root, model.CreateEmbedding(command, false).First(), model);

                Console.WriteLine($"{hit.Score} {hit.Node}");
            }
        }
    }
}