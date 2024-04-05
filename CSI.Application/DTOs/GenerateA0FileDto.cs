using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class GenerateA0FileDto
    {
        public string? Path { get; set; } = string.Empty;
        public AnalyticsParamsDto? analyticsParamsDto { get; set; }
    }
}
