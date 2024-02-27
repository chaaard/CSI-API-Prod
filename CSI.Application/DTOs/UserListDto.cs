using CSI.Domain.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class UserListDto : BaseEntity
    {
        public string EmployeeNumber { get; set; } = string.Empty;
        public string FirstName { get; set; } = string.Empty;
        public string LastName { get; set; } = string.Empty;
        public string Username { get; set; } = string.Empty;
        public string Role { get; set; } = string.Empty;
        public string Club { get; set; } = string.Empty;
        public bool IsLogin { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public string? SearchQuery { get; set; }
        public string? ColumnToSort { get; set; }
        public string? OrderBy { get; set; }
    }
}
