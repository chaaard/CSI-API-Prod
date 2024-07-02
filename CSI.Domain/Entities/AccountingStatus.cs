using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingStatus
    {
        public int Id { get; set; }
        public string StatusName { get; set; } = string.Empty;
        public bool DeleteFlag { get; set; }
    }
}
