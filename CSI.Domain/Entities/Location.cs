using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class Location
    {
        public int Id { get; set; }
        public int LocationCode { get; set; }
        public string LocationName { get; set; } = string.Empty;
        public string ShortName { get; set; } = string.Empty;
        public string? VendorCode { get; set; } = string.Empty;
        public bool DeleteFlag { get; set; }
    }
}
