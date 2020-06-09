using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Quartz.Simpl;
using Quartz.Util;

namespace Quartz.Spi.CosmosDbJobStore.Util
{
    /// <inheritdoc />
    /// <summary>
    /// We want to use Quartz.Simpl.JsonObjectSerializer but we have to inherit it to access its protected methods.
    /// </summary>
    public class InheritedJsonObjectSerializer : JsonObjectSerializer
    {
        public new JsonSerializerSettings CreateSerializerSettings()
        {
            var settings = base.CreateSerializerSettings();
            
            settings.Converters.Add(new TimeOfDayConverter());
            settings.Converters.Add(new TypeConverter());
            settings.TypeNameHandling = TypeNameHandling.All;
            settings.DateFormatHandling = DateFormatHandling.IsoDateFormat;
            settings.DateParseHandling = DateParseHandling.DateTimeOffset;
            settings.DateTimeZoneHandling = DateTimeZoneHandling.Utc; // This is important, as DateTimeOffset used in queries is parsed as UTC!
            
            return settings;
        }
        
        
        protected class TimeOfDayConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var tod = (TimeOfDay) value;
                writer.WriteStartObject();

                writer.WritePropertyName("$type");
                writer.WriteValue(value.GetType().AssemblyQualifiedNameWithoutVersion());

                writer.WritePropertyName("Hour");
                writer.WriteValue(tod.Hour);

                writer.WritePropertyName("Minute");
                writer.WriteValue(tod.Minute);

                writer.WritePropertyName("Second");
                writer.WriteValue(tod.Second);
                
                writer.WriteEndObject();
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                var jObject = JObject.Load(reader);
                
                var hour = jObject["Hour"].Value<int>();
                var minute = jObject["Minute"].Value<int>();
                var second = jObject["Second"].Value<int>();

                return new TimeOfDay(hour, minute, second);
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof(TimeOfDay) == objectType;
            }
        }

        public class TypeConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                Type typeValue = (Type) value;
                writer.WriteValue(typeValue.FullName + ", " + typeValue.Assembly.GetName().Name);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer) =>
                throw new NotImplementedException();

            public override bool CanRead => false;

            public override bool CanConvert(Type objectType) => typeof(Type).IsAssignableFrom(objectType);
        }
    }
}