using System;
using System.Collections.Generic;
using System.IO.Enumeration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class RefreshAnalyticsDto
    {
        public List<DateTime> dates { get; set; } = new List<DateTime>();
        public List<string> memCode { get; set; } = new List<string>();
        public string? userId { get; set; } = string.Empty;
        public List<int> storeId { get; set; } = new List<int>();
        public string? action { get; set; } = string.Empty;
        public string? fileName { get; set; } = string.Empty;
        public string? remarks { get; set; } = string.Empty;
        public string? transactionNo { get; set; } = string.Empty;
        public string? regNo { get; set; } = string.Empty;
    }
}
