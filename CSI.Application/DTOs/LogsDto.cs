using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class LogsDto
    {
        public string Username { get; set; } = string.Empty;
        public DateTime Date { get; set; }
        public string Action { get; set; } = string.Empty;
        public string? Remarks { get; set; } = string.Empty;
        public int? RowsCountBefore { get; set; }
        public int? RowsCountAfter { get; set; }
        public decimal? TotalAmount { get; set; }
        public int Club { get; set; }
        public string? Filename { get; set; } = string.Empty;
        public int? ActionId { get; set; }
        public int? AnalyticsId { get; set; }
        public string? OldValue { get; set; } = string.Empty;
        public string? NewValue { get; set; } = string.Empty;
    }
}
