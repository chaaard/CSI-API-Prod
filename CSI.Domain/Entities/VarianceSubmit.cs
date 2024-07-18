using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class VarianceSubmit
    {
        public decimal MMS { get; set; }
        public decimal CSI { get; set; }
        public decimal Variance { get; set; }
        public int CategoryId { get; set; }
        public string CategoryName { get; set; } = string.Empty;
        public int LocationId { get; set; }
        public string TransactionDate { get; set; } = string.Empty;
    }
}
