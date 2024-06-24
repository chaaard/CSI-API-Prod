using CSI.Application.Interfaces;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    public class CategoryController : ControllerBase
    {
        public readonly ICategoryService _categoryService;
        public CategoryController(ICategoryService categoryService)
        {
            _categoryService = categoryService;
        }

        [HttpPost("GetCategory")]
        public async Task<IActionResult> GetCategory()
        {
            var result = await _categoryService.GetCategory();

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }
    }
}
