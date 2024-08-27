using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class FloatingCSIDto
    {
        public int Id { get; set; }
        public string? CustomerCode { get; set; }
        public string? UserId { get; set; }
        public string? StoreId { get; set; }
        public string? OrderNo { get; set; } = string.Empty;
        public RefreshAnalyticsDto? refreshAnalyticsDto { get; set; }
    }
}
