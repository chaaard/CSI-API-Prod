using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Application.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    //[Authorize]
    public class ProofListController : ControllerBase
    {
        public readonly IProofListService _proofListService;

        public ProofListController(IProofListService proofListService)
        {
            _proofListService = proofListService;
        }

        [HttpPost("UploadProofList")]
        public async Task<IActionResult> UploadProofList(List<IFormFile> files, [FromForm] string customerName, [FromForm] string strClub, [FromForm] string selectedDate, [FromForm] string analyticsParamsDto)
        {
            try
            {
                var result =  await _proofListService.ReadProofList(files, customerName, strClub, selectedDate, analyticsParamsDto);
                return Ok(result);
            }
            catch (OperationCanceledException)
            {
                return StatusCode(499, "Request canceled");
            }
            catch (Exception ex)
            {
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GetPortal")]
        public async Task<IActionResult> GetPortal(PortalParamsDto portalParamsDto)
        {
            try
            {
                var result = await _proofListService.GetPortal(portalParamsDto);

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
