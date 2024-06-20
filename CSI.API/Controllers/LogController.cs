using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Application.Services;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    public class LogController : ControllerBase
    {
        private readonly ILogService _logService;

        public LogController(ILogService logService)
        {
            _logService = logService;
        }

        [HttpPost("GetLogsListAsync")]
        public async Task<IActionResult> GetLogsListAsync(PaginationDto pagination)
        {
            var result = await _logService.GetLogsListAsync(pagination);

            if (result.Item1 != null)
            {
                var data = new
                {
                    LogList = result.Item1,
                    TotalPages = result.Item2
                };

                return Ok(data);
            }
            return NotFound();
        }

        [HttpGet("GetLogByIdAsync")]
        public async Task<IActionResult> GetLogByIdAsync(string id)
        {
            var result = await _logService.GetLogByIdAsync(id);

            if (result != null)
            {
                return Ok(result);
            }
            return NotFound();
        }
    }
}
