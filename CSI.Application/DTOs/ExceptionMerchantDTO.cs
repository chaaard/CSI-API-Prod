using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class ExceptionMerchantDTO
    {
        public int analyticId { get; set; }
        public List<string> memCode { get; set; } = new List<string>();
        public List<int> storeId { get; set; } = new List<int>();
        public string? userId { get; set; } = string.Empty;
    }
}
