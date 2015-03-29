using System;

namespace Simulation
{
    class Program
    {
        static void Main(string[] args)
        {
            var simulator = new Simulator
                            {
                                NumberOfWorkers = 15
                            };
            simulator.Run();
            Console.ReadLine();
        }
    }
}
