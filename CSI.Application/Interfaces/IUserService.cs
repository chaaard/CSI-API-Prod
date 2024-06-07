using CSI.Application.DTOs;
using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Interfaces
{
    public interface IUserService
    {
        Task<UserDto> AuthenticateAsync(string username, string password);
        Task<UserDto> AuthenticateADAsync();
        Task<UserDto> Logout(string username);
        Task<UserInfoDto> GetUserInfo(string username);
        Task<bool?> IsLogin(string username);
        
        Task<UserDto> ChangePassword(string username, string password);
        Task<UserDto> LoginAttempt(string username);

        Task<(List<UserDto>, int totalPages)> GetUsersListAsync(PaginationDto pagination);
        Task<User> GetUserByIdAsync(string id);
        Task<User> UpdateUserByIdAsync(User id);
        Task<User> InsertUserAsync(User id);
        Task<List<User>> GetUsersFullListAsync();
        Task<bool> ResetUserPasswordByIdAsync(string id);
    }
}
