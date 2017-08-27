using System;
using System.Text;

namespace PgDbfLib
{
    public class DbfColumn
    {
        private const int JulianOffset = 1721425;
        public bool Export { get; set; }
        public int Offset { get; set; }
        public virtual int Length
        {
            get
            {
                return length;
            }
            set
            {
                length = value;
                RawValue = new byte[value];
            }
        }
        private int length;
        public char Type
        {
            get
            {
                return type;
            }
            set
            {
                type = value;
                switch (value)
                {
                    case 'C':
                    case 'N':
                    case 'F':
                        Transform = () => Encoding.ASCII.GetString(RawValue).Trim();
                        break;
                    case 'Y':
                        Transform = () => ((decimal)BitConverter.ToInt64(RawValue, 0) / 10000).ToString();
                        break;
                    case 'D':
                        Transform = () => $"{Encoding.ASCII.GetString(RawValue, 0, 4)}-{Encoding.ASCII.GetString(RawValue,4,2)}-{Encoding.ASCII.GetString(RawValue,6,2)}";
                        break;
                    case 'T':
                        Transform = () =>
                        {
                            var julianDays = BitConverter.ToInt32(RawValue, 0);
                            if (julianDays == 0)
                            {
                                return string.Empty;
                            }
                            return DateTime.MinValue.AddDays(julianDays - JulianOffset).AddMilliseconds(BitConverter.ToInt32(RawValue, 4)).ToString();
                        };
                        break;
                    case 'B':
                        Transform = () => BitConverter.ToDouble(RawValue, 0).ToString();
                        break;
                    case 'I':
                        Transform = () => BitConverter.ToInt32(RawValue, 0).ToString();
                        break;
                    case 'L':
                        Transform = () => { var bVal = Convert.ToChar(RawValue[0]); return bVal == 'Y' || bVal == 'T' ? "t" : "f"; };
                        break;
                    case 'M':
                        break;
                    default:
                        Transform = () => string.Empty;
                        break;
                }
            }
        }
        private char type;
        protected byte[] RawValue;
        protected Func<string> Transform;
        public string ConvertRawValue(byte[] rawRow)
        {
            Array.Copy(rawRow, Offset, RawValue, 0, Length);
            return Transform();
        }
    }
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       