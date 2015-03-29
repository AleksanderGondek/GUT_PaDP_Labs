using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace Simulation
{
    sealed internal class Simulator
    {
        public int NumberOfWorkers { get; set; }
        private ConcurrentDictionary<int, MyMessage> _messageBus;

        public void Run()
        {
            if (NumberOfWorkers <= 0)
            {
                return;
            }

            PrepareForSimulation();

            var generationsNeeded = GetNumberOfGenerationsNeeded();
            for (var i = 0; i <= generationsNeeded; i++) //Inclusive!!
            {
                var i1 = i;
                Parallel.For(0, NumberOfWorkers, x =>
                                                 {
                                                     var worker = new Worker(x, NumberOfWorkers, i1, _messageBus);
                                                     worker.Run();
                                                 });
                //for (var j = 0; j < NumberOfWorkers; j++)
                //{
                //    var worker = new Worker(j, NumberOfWorkers, i1, _messageBus);
                //    worker.Run();
                //}
            }
        }

        private void PrepareForSimulation()
        {
            if (_messageBus != null)
            {
                _messageBus.Clear();
                _messageBus = null;
            }

            _messageBus = new ConcurrentDictionary<int, MyMessage>();
            for (var i = 0; i < NumberOfWorkers; i++)
            {
                _messageBus.TryAdd(i, null);
            }
        }

        private int GetNumberOfGenerationsNeeded()
        {
            var generationsNeeded = Math.Log(NumberOfWorkers, 2.0d);
            generationsNeeded = Math.Floor(generationsNeeded);
            
            return Convert.ToInt32(generationsNeeded);
        }
    }
}
