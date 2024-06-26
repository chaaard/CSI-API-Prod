using CSI.Domain.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class CustomerCodes
    {
        public int Id { get; set; }
        public string CustomerNo { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public string CustomerCode { get; set; } = string.Empty;
        public int? CategoryId { get; set; }
        public bool DeleteFlag { get; set; }
    }
}
