using CSI.Application.Interfaces;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    public class MerchantController : ControllerBase
    {
        public readonly IMerchantService _merchantService;
        public MerchantController(IMerchantService merchantService)
        {
            _merchantService = merchantService;
        }

        [HttpPost("GetMerchant")]
        public async Task<IActionResult> GetMerchant()
        {
            var result = await _merchantService.GetMerchant();

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetAllMerchant")]
        public async Task<IActionResult> GetAllMerchant()
        {
            var result = await _merchantService.GetAllMerchant();

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }
    }
}
