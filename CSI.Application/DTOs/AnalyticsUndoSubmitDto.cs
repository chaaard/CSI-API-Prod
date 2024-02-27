using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AnalyticsUndoSubmitDto
    {
        public string date { get; set; } = string.Empty;
        public string memCode { get; set; } = string.Empty;
        public int storeId { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
    }
}
