using Bogus;

namespace OrderService.Models;

public class Order
{
    public int Id { get; set; } 
    public string? Description { get; set; }
    public decimal Price { get; set; }= 0;
    public decimal PriceTotal { get; set; } = 0;
    public int CustomerId { get; set; }
    public string Product { get; set; }
}


public class FakeOrderGenerator
{
    public static List<Order> Generate(int count, List<int> validCustomerIds)
    { 
        var orderFaker = new Faker<Order>() 
            .RuleFor(o => o.Id, f => f.IndexFaker + 1) 
            .RuleFor(o => o.Description, f => f.Commerce.ProductDescription()) 
            .RuleFor(o => o.Price, f => Math.Round(f.Finance.Amount(5.00m, 500.00m), 2)) 
            .RuleFor(o => o.PriceTotal, (f, o) => Math.Round(o.Price * 1.10m, 2)) 
            .RuleFor(o => o.CustomerId, f => f.PickRandom(validCustomerIds))
            .RuleFor(o => o.Product, f => f.Commerce.ProductName());

        return orderFaker.Generate(count);
    }
}
