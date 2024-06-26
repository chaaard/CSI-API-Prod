using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class CategoryCode
    {
        public int CategoryId { get; set; }
        public string CustomerCodes { get; set; } = string.Empty;
        public string CategoryName { get; set; } = string.Empty;
        public int IsVisible { get; set; }
    }
}
