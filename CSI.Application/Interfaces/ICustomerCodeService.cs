using CSI.Application.DTOs;
using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Interfaces
{
    public interface ICustomerCodeService
    {
        Task<(List<CustomerCodeDto>, int totalPages)> GetCustomerCodesAsync(PaginationDto pagination);
        Task<List<CustomerCodeDto>> GetCustomerCodesByCategory(PaginationDto pagination);
        Task<CustomerCodes> GetCustomerCodeByIdAsync(int Id);
        Task<CustomerCodes> InsertCustomerCodeAsync(CustomerCodeParamsDto customerCode);
        Task<CustomerCodes> UpdateCustomerCodeByIdAsync(CustomerCodeParamsDto customerCode);
        Task<bool> DeleteCustomerCodeByIdAsync(int Id);
        Task<List<CustomerCodes>> GetCustomerDdCodesAsync();
    }
}
