using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class PortalParamsDto
    {
        public List<DateTime> dates { get; set; } = new List<DateTime>();
        public List<string> memCode { get; set; } = new List<string>();
        public string? userId { get; set; } = string.Empty;
        public string? orderNo { get; set; } = string.Empty;
        public List<int> storeId { get; set; } = new List<int>();
    }
}
