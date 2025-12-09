// See https://aka.ms/new-console-template for more information

using System;

using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Types;
using Spectre.Console.Cli;
using ParquetSharp;
using ParquetSharp.Arrow;

namespace PTPRepro
{
    internal static class Program
    {
        public async static Task<int> Main(string[] args)
        {
            var app = new CommandApp();

            app.Configure(c =>
            {
                c.UseStrictParsing();
                c.AddCommand<Tester>("test");
            });

            return await app.RunAsync(args);
        }
    }
}


