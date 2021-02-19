using System.IO;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Quartz.Spi.CosmosDbJobStore.Util
{
    public class QuartzCosmosSerializer : CosmosSerializer
    {
        private readonly JsonSerializer _serializer;
        
        public QuartzCosmosSerializer()
        {
            var inheritedJsonSerializer = new InheritedJsonObjectSerializer();
            
            _serializer = JsonSerializer.Create(inheritedJsonSerializer.CreateSerializerSettings());
        }

        public override T FromStream<T>(Stream stream)
        {
            using var sr = new StreamReader(stream);
            return (T) _serializer.Deserialize(sr, typeof(T));
        }

        public override Stream ToStream<T>(T input)
        {
            var ms = new MemoryStream();
            var sw = new StreamWriter(ms);
            
            _serializer.Serialize(sw, input, typeof(T));
            sw.Flush();
            
            return ms;
        }
    }
}