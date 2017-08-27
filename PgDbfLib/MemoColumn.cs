using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace PgDbfLib
{
    public class MemoColumn : DbfColumn
    {
        public FileStream MemoFile { get; set; }
        public int MemoBlockSize { get; set; }

        public override int Length {
            get => base.Length;
            set
            {
                base.Length = value;
                if (value == 10)
                {
                    Transform = ReadDbtMemo;
                }
                else if (value == 4)
                {
                    Transform = ReadFptMemo;
                }
            }
        }

        private static byte MemoTerminator = 0x1a;

        private string ReadFptMemo()
        {
            var blockOffset = BitConverter.ToInt32(RawValue, 0) * MemoBlockSize;
            if (blockOffset == 0)
            {
                return string.Empty;
            }
            MemoFile.Seek(blockOffset, SeekOrigin.Begin);
            var memoHeader = new byte[8];
            MemoFile.Read(memoHeader, 0, 8);
            //var memoSize = BitConverter.ToInt32(memoHeader, 4);
            int memoSize = memoHeader[4] << 24 | memoHeader[5] << 16 | memoHeader[6] << 8 | memoHeader[7];
            var memo = new byte[memoSize];
            MemoFile.Read(memo, 0, memoSize);
            return Encoding.ASCII.GetString(memo);
        }

        private string ReadDbtMemo()
        {
            var blockOffset = Convert.ToInt64(Encoding.ASCII.GetString(RawValue)) * MemoBlockSize;
            MemoFile.Seek(blockOffset, SeekOrigin.Begin);
            var memo = new List<byte>();
            byte[] memoBlock;
            int terminatorIndex;
            do
            {
                terminatorIndex = memo.Count;
                //if we're not on the first block, go back one in case the last byte of the previous block was the first terminator
                if (terminatorIndex > 0)
                {
                    --terminatorIndex;
                }
                memoBlock = new byte[MemoBlockSize];
                MemoFile.Read(memoBlock, 0, MemoBlockSize);
                memo.AddRange(memoBlock);
                //Check block for two terminating characters; This signifies the end of the memo
                do
                {
                    terminatorIndex = memo.IndexOf(MemoTerminator, terminatorIndex) + 1;
                } while (terminatorIndex > 0 && terminatorIndex < memo.Count && memo[terminatorIndex] != MemoTerminator);
            } while (terminatorIndex <= 0 || terminatorIndex == memo.Count);

            return Encoding.ASCII.GetString(memo.ToArray(),0,terminatorIndex-1);
        }
    }
}
