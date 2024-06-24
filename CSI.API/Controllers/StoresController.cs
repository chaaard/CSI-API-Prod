using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Application.Services;
using CSI.Domain.Entities;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    public class StoresController : ControllerBase
    {
        public readonly ILocationService _LocationService;
        public StoresController(ILocationService locationService)
        {
            _LocationService = locationService;
        }

        [HttpGet("GetLocationByIdAsync")]
        public async Task<IActionResult> GetLocationByIdAsync(int Id)
        {
            var result = await _LocationService.GetLocationByIdAsync(Id);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpGet("GetLocationDdCodesAsync")]
        public async Task<IActionResult> GetLocationDdCodesAsync()
        {
            var result = await _LocationService.GetLocationDdCodesAsync();

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetLocationsAsync")]
        public async Task<IActionResult> GetLocationsAsync(PaginationDto pagination)
        {
            var result = await _LocationService.GetLocationsAsync(pagination);

            if (result.Item1 != null)
            {
                var data = new
                {
                    LocationList = result.Item1,
                    TotalPages = result.Item2
                };

                return (Ok(data));
            }
            return (NotFound());
        }

        [HttpPost("InsertLocationAsync")]
        public async Task<IActionResult> InsertLocationAsync(LocationDto location)
        {
            var result = await _LocationService.InsertLocationAsync(location);

            if (result != null)
            {
                return (Ok(result));
            }
            return NotFound();
        }

        [HttpPut("UpdateLocationByIdAsync")]
        public async Task<IActionResult> UpdateLocationByIdAsync(LocationDto location)
        {
            var result = await _LocationService.UpdateLocationByIdAsync(location);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPut("DeleteLocationByIdAsync")]
        public async Task<IActionResult> DeleteLocationByIdAsync(LocationDto location)
        {
            var result = await _LocationService.DeleteLocationByIdAsync(location);

            return (Ok(result));
        }
    }
}
