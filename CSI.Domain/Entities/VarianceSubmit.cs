using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class VarianceSubmit
    {
        public decimal CategoryId { get; set; }
        public string CustomerCodes { get; set; } = string.Empty;
        public string CategoryName { get; set; } = string.Empty;
        public decimal MMS { get; set; }
        public decimal Variance { get; set; }
        public decimal CSI { get; set; }
        public int Status { get; set; }
    }
}
