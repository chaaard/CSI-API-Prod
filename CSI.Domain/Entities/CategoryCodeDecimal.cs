using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class CategoryCodeDecimal
    {
        public decimal CategoryId { get; set; }
        public string CustomerCodes { get; set; } = string.Empty;
        public string CategoryName { get; set; } = string.Empty;
        public int IsVisible { get; set; }
    }
}
