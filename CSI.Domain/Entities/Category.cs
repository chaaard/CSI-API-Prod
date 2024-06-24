using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class Category
    {
        public int Id { get; set; }
        public string CategoryName { get; set; } = string.Empty;
        public int StatusId { get; set; }
        public bool DeleteFlag { get; set; }
        public bool IsVisible { get; set; }
    }
}
