using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    public class CreditMemoController: ControllerBase
    {
        public readonly ICreditMemoService _creditMemoService;

        public CreditMemoController(ICreditMemoService creditMemoService)
        {
            _creditMemoService = creditMemoService;
        }

        [HttpPost("GetCMVariance")]
        public async Task<IActionResult> GetCMVariance(VarianceParams variance)
        {
            try
            {
                var result = await _creditMemoService.GetCMVariance(variance);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }
        [HttpPut("UpdateCustCreditMemo")]
        public async Task<IActionResult> UpdateCustCreditMemo(CustomerTransactionDto req)
        {
            var result = await _creditMemoService.UpdateCustCreditMemo(req);
            if(result)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPut("UpdateCreditMemoStatus")]
        public async Task<IActionResult> UpdateCreditMemoStatus(CreditMemoDto req)
        {
            var result = await _creditMemoService.UpdateCreditMemoStatus(req);
            if (result)
            {
                return (Ok(result));
            }
            return (NotFound());
        }
        [HttpPost("RetrieveUpdateCreditMemoData")]
        public async Task<IActionResult> RetriveCreditMemo(VarianceParams variance)
        {
            try
            {
                var result = await _creditMemoService.RetrieveUpdateCreditMemoData(variance);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }
        [HttpPost("GetCreditMemoInvoice")]
        public async Task<IActionResult> GetCreditMemoInvoice(CreditMemoInvoiceDto req)
        {
            try
            {
                var result = await _creditMemoService.GetCreditMemoInvoice(req);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

    }
}
