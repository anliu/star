using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Star.Layoff.DtsComponents.Utilities
{
    public static class Helpers
    {
        public static byte[] EncodePrefixedValue(string val)
        {
            if (string.IsNullOrEmpty(val))
            {
                return null;
            }

            if (val.StartsWith("Base64:"))
            {
                return Convert.FromBase64String(val.Substring("Base64:".Length));
            }
            else if (val.StartsWith("Utf8:"))
            {
                // in case some keys do start with "Base64:", need a way to
                // escape that
                return Encoding.UTF8.GetBytes(val.Substring("Utf8:".Length));
            }
            else
            {
                // default is Utf8 encoding
                return Encoding.UTF8.GetBytes(val);
            }
        }
    }
}
