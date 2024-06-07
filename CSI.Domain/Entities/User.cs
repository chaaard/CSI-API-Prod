using CSI.Domain.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class User : BaseEntity
    {
        public string EmployeeNumber { get; set; } = string.Empty;
        public string FirstName { get; set; } = string.Empty;
        public string? MiddleName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string Salt { get; set; } = string.Empty;
        public string Hash { get; set; } = string.Empty;
        public int RoleId { get; set; }
        public int Club { get; set; }
        public bool IsLogin { get; set; }
        public bool IsFirstLogin { get; set; }
        public bool Status { get; set; }
        public int Attempt {  get; set; }
    }
}
