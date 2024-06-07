using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Application.Services;
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
    public class UsersController : ControllerBase
    {
        private readonly IUserService _userService;

        public UsersController(IUserService userListService)
        {
            _userService = userListService;
        }

        [HttpGet("GetUserByIdAsync")]
        public async Task<IActionResult> GetUserByIdAsync(string user)
        {
            var result = await _userService.GetUserByIdAsync(user);

            if (result != null)
            {
                return Ok(result);
            }
            return NotFound();
        }

        [HttpGet("GetUsersFullListAsync")]
        public async Task<IActionResult> GetUsersFullListAsync()
        {
            var userListDto = await _userService.GetUsersFullListAsync();

            if (userListDto != null)
            {
                return Ok(userListDto);
            }
            return NotFound();
        }

        [HttpPost("GetUsersListAsync")]
        public async Task<IActionResult> GetUsersListAsync(PaginationDto pagination)
        {
            var result = await _userService.GetUsersListAsync(pagination);

            if (result.Item1 != null)
            {
                var data = new
                {
                    CustomerCodesList = result.Item1,
                    TotalPages = result.Item2
                };

                return Ok(data);
            }
            return NotFound();
        }

        [HttpPost("InsertUserAsync")]
        public async Task<IActionResult> InsertUserAsync(User user)
        {
            var result = await _userService.InsertUserAsync(user);

            if (result != null)
            {
                return (Ok(result));
            }
            return NotFound();
        }

        [HttpPut("UpdateUserByIdAsync")]
        public async Task<IActionResult> UpdateUserByIdAsync(User user)
        {
            var result = await _userService.UpdateUserByIdAsync(user);

            if (result != null)
            {
                return (Ok(result));
            }
            return NotFound();
        }

        [HttpPut("ResetUserPasswordByIdAsync")]
        public async Task<IActionResult> ResetUserPasswordByIdAsync(string id)
        {
            var result = await _userService.ResetUserPasswordByIdAsync(id);
            
            return (Ok(result));
        }
    }
}
