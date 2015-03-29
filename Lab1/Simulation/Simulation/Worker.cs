using System;
using System.Collections.Concurrent;

namespace Simulation
{
    internal sealed class Worker
    {
        private readonly int _id;
        private readonly int _totalWorkersNumber;
        private readonly int _generation;
        private MyMessage _message;
        private readonly ConcurrentDictionary<int, MyMessage> _messageBus;

        public Worker(int id, int totalWorkersNumber, int generation, ConcurrentDictionary<int, MyMessage> messageBus)
        {
            _id = id;
            _totalWorkersNumber = totalWorkersNumber;
            _generation = generation;
            _messageBus = messageBus;

            //Console.WriteLine("Worker {0} (out of {1}) created. Generation {2}.", _id, _totalWorkersNumber, _generation);
        }

        public void Run()
        {
            //Console.WriteLine("Worker {0} started.", _id);

            if (CanISendMessage())
            {
                HandleRoot();
                HandleLeaves();
            }

            //Console.WriteLine("Worker {0} terminated.", _id);
        }

        private void HandleRoot()
        {
            if (_id != 0)
            {
                return;
            }

            MyMessage storedMessage;
            _messageBus.TryGetValue(_id, out storedMessage);
            if (storedMessage == null)
            {
                var startMessage = new MyMessage {FromWhom = _id, ToWhom = _id, Text = @"Test message"};
                _messageBus.TryUpdate(_id, startMessage, null);
                _message = startMessage;
            }
            else
            {
                _message = storedMessage;
            }

            SendMessage(GetSenderIndex());
        }

        private void HandleLeaves()
        {
            if (_id == 0)
            {
                return;
            }

            if (ReceiveMessage())
            {
                SendMessage(GetSenderIndex());
            }
        }

        private int GetSenderIndex()
        {
            var index = _id + Convert.ToInt32(Math.Pow(2.0d, _generation));
            if (index >= _totalWorkersNumber)
            {
                return -1;
            }

            return index;
        }

        private bool ReceiveMessage()
        {
            MyMessage storedMessage;
            _messageBus.TryGetValue(_id, out storedMessage);
            if (storedMessage == null)
            {
                return false;
            }

            _message = storedMessage;
            //Console.WriteLine("Worker {0} received message >>{1}<< from worker {2}", _id, _message.Text, _message.FromWhom);
            return true;
        }

        private void SendMessage(int toWhom)
        {
            if (toWhom < 0)
            {
                return;
            }

            if (_id.Equals(toWhom))
            {
                throw new Exception(string.Format("Worker {0} tried to send message to self!", _id));
            }

            Console.WriteLine(
                _messageBus.TryUpdate(toWhom, _message, null) ? "Worker {0} send message >>{1}<< to worker {2}"
                : "Worker {0}  could not send message >>{1}<< to worker {2}", _id, _message.Text, toWhom);
        }

        private bool CanISendMessage()
        {
            var lastIndexToSend = Convert.ToInt32(Math.Pow(2.0d, _generation)) -1;
            return _id <= lastIndexToSend;
        }
    }
}
