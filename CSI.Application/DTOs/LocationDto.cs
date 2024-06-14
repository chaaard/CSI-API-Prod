using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class LocationDto
    {
        public int Id { get; set; }
        public int LocationCode { get; set; }
        public string LocationName { get; set; } = string.Empty;
        public string ShortName { get; set; } = string.Empty;
        public bool DeleteFlag { get; set; }
        public string? UserId { get; set; } = string.Empty;
    }
}
