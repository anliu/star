using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Star.Layoff.DtsComponents.Utilities
{
    internal class JsonParser
    {
        public static IEnumerable<string> GetPropertyList(string json)
        {
            var paths = new List<string>();
            var obj = JObject.Parse(json);
            TraverseObject(obj, (token) => {
                if (token.Type == JTokenType.Property && token.First != null && token.First.Type == JTokenType.Object ||
                    token.Type == JTokenType.Object && token.HasValues)
                {
                    // traverse only
                }
                else
                {
                    paths.Add(token.Path);
                }
            });
            return paths;
        }

        private static void TraverseObject(JToken obj, Action<JToken> tokenHandler)
        {
            var cur = obj.First;
            tokenHandler(obj);
            while (cur != null)
            {
                tokenHandler(cur);
                if (cur.First != null && cur.First.Type == JTokenType.Object)
                {
                    TraverseObject(cur.First, tokenHandler);
                }
                cur = cur.Next;
            }
        }
    }
}
