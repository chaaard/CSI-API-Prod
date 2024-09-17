using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class TempVwMMSDto
    {
        public decimal CSSTOR { get; set; }
        public decimal CSCEN { get; set; }
        public decimal CSDATE { get; set; }
        public decimal CSREG { get; set; }
        public decimal CSTIL { get; set; }
        public decimal CSRPAM { get; set; }
        public string? CSSCUR {  get; set; }
        public string? CSTCUR { get; set; }
        public decimal CSTRAT { get; set; }
        public string? CSTMD { get; set; }
        public decimal CSTRAM { get; set; }

    }
}
