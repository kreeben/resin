namespace Resin.CommandLine
{
    public static class ArgumentParser
    {
        public static IDictionary<string, string> ParseArgs(string[] args)
        {
            var dic = new Dictionary<string, string>();
            for (int i = 1; i < args.Length; i += 2)
            {
                var key = args[i].Replace("--", "");
                var value = args[i + 1];

                if (value.StartsWith("--"))
                {
                    dic.Add(key, "true");
                    i--;
                }
                else
                {
                    dic.Add(key, i == args.Length - 1 ? null : value);
                }
            }
            return dic;
        }
    }
}
