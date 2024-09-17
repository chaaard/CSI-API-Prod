using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class VarianceMMS
    {
        public decimal? MMS { get; set; }
        public decimal? CSI { get; set; }
        public decimal? Variance { get; set; }
    }

    public class VarianceParams
    {
        public string? CurrentDate { get; set; }
        public int Store { get; set; }
        public string? TranType { get; set; }
        public string? searchQuery { get; set; }
    }
}
