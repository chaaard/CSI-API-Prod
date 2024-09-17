using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class BaseModel
    {
        public int? Id { get; set; }
        public decimal? Amount { get; set; }
        public int? Status { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public string? ModifiedBy { get; set; }
        public int? Club { get; set; }
        public long? Seq { get; set; }
    }
}
