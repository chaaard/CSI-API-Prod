using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class DashboardAccounting
    {
        public string LocationName { get; set; } = string.Empty;
        public int? GrabMart { get; set; }
        public int? GrabFood { get; set; }
        public int? PickARooMerch { get; set; }
        public int? PickARooFS { get; set; }
        public int? FoodPanda { get; set; }
        public int? MetroMart { get; set; }
        public int? LocationCode { get; set; }
    }
}
