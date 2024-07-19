using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class VarianceMMSCSIDto
    {
        public int CategoryId { get; set; }
        public string CategoryName { get; set; } = string.Empty;
        public int LocationId { get; set; }
        public string TransactionDate { get; set; } = string.Empty;
        public decimal MMS { get; set; }
        public decimal CSI { get; set; }
        public decimal Variance { get; set; }
    }
}
