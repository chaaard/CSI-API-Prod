using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    //[Authorize]
    public class CustomerCodeController : ControllerBase
    {
        public readonly ICustomerCodeService _customerCodeService;

        public CustomerCodeController(ICustomerCodeService customerCodeService)
        {
            _customerCodeService = customerCodeService;
        }

        [HttpGet("GetCustomerCodeByIdAsync")]
        public async Task<IActionResult> GetCustomerCodeByIdAsync(int Id)
        {
            var result = await _customerCodeService.GetCustomerCodeByIdAsync(Id);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpGet("GetCustomerDdCodesAsync")]
        public async Task<IActionResult> GetCustomerDdCodesAsync()
        {
            var result = await _customerCodeService.GetCustomerDdCodesAsync();

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetCustomerCodesAsync")]
        public async Task<IActionResult> GetCustomerCodesAsync(PaginationDto pagination)
        {
            var result = await _customerCodeService.GetCustomerCodesAsync(pagination);

            if (result.Item1 != null)
            {
                var data = new
                {
                    CustomerCodesList = result.Item1,
                    TotalPages = result.Item2
                };

                return (Ok(data));
            }
            return (NotFound());
        }

        [HttpPut("UpdateCustomerCodeByIdAsync")]
        public async Task<IActionResult> UpdateCustomerCodeByIdAsync(CustomerCodes customerCode)
        {
            var result = await _customerCodeService.UpdateCustomerCodeByIdAsync(customerCode);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPut("DeleteCustomerCodeByIdAsync")]
        public async Task<IActionResult> DeleteCustomerCodeByIdAsync(int Id)
        {
            var result = await _customerCodeService.DeleteCustomerCodeByIdAsync(Id);

            return (Ok(result));
        }
    }
}
