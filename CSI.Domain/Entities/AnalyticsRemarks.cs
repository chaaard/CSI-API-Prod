using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AnalyticsRemarks
    {
        public int Id { get; set; }
        public int AnalyticsId { get; set; }
        public string Remarks { get; set; } = string.Empty;
    }
}
