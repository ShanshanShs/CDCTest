// See https://aka.ms/new-console-template for more information

using CDCTest.MySql;

Console.WriteLine("BinLogReader start");

var binLogReader = new BinLogReader();
await binLogReader.Start();

Console.WriteLine("BinLogReader exit.");
Console.ReadLine();