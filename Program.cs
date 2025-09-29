using Microsoft.EntityFrameworkCore;
using System.Text.Json;
using System.Text.Json.Serialization;

var builder = WebApplication.CreateBuilder(args);

// ✅ Supabase connection string
var connectionString = Environment.GetEnvironmentVariable("SUPABASE_CONNECTION")
    ?? builder.Configuration.GetConnectionString("SupabaseConnection");

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(connectionString));

var app = builder.Build();

// ✅ Health Check
app.MapGet("/", async (AppDbContext db) =>
{
    try
    {
        await db.Database.OpenConnectionAsync();
        await db.Database.CloseConnectionAsync();
        return Results.Ok("✅ Supabase DB connected successfully!");
    }
    catch (Exception ex)
    {
        return Results.Problem($"❌ Connection failed: {ex.Message}");
    }
});

// ✅ GET: All users
app.MapGet("/cashKuber", async (AppDbContext db) =>
{
    var users = await db.MoneyViewUsers.ToListAsync();
    return Results.Json(users);
});

// ✅ POST: Insert user(s) - FIXED VERSION WITH CASE-INSENSITIVE JSON
app.MapPost("/cashKuber", async (HttpContext http, AppDbContext db) =>
{
    // Check API key
    if (!http.Request.Headers.TryGetValue("api-key", out var apiKey) || apiKey != "moneyview")
        return Results.Json(new { message = "Unauthorized: Invalid api-key" }, statusCode: 401);

    List<MoneyViewUser> users;

    // ✅ JSON options with case-insensitive property matching
    var jsonOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    try
    {
        // Read raw body
        using var reader = new StreamReader(http.Request.Body);
        var body = await reader.ReadToEndAsync();

        Console.WriteLine($"📥 Received body: {body}"); // Debug log

        if (string.IsNullOrWhiteSpace(body))
            return Results.Json(new { message = "Empty request body" }, statusCode: 400);

        // Try to parse as array first
        try
        {
            users = JsonSerializer.Deserialize<List<MoneyViewUser>>(body, jsonOptions) ?? new List<MoneyViewUser>();
            Console.WriteLine($"✅ Parsed {users.Count} users"); // Debug log
        }
        catch
        {
            // If array parsing fails, try single object
            var singleUser = JsonSerializer.Deserialize<MoneyViewUser>(body, jsonOptions);
            users = singleUser != null ? new List<MoneyViewUser> { singleUser } : new List<MoneyViewUser>();
            Console.WriteLine($"✅ Parsed 1 user (single object)"); // Debug log
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Parse error: {ex.Message}"); // Debug log
        return Results.Json(new { message = $"Invalid JSON: {ex.Message}" }, statusCode: 400);
    }

    if (users.Count == 0)
        return Results.Json(new { message = "No valid users provided" }, statusCode: 400);

    var inserted = new List<object>();
    var skipped = new List<object>();

    // Get all existing phones and PANs in one query for efficiency
    var existingPhones = await db.MoneyViewUsers
        .Where(u => u.Phone != null)
        .Select(u => u.Phone)
        .ToListAsync();

    var existingPans = await db.MoneyViewUsers
        .Where(u => u.Pan != null)
        .Select(u => u.Pan)
        .ToListAsync();

    foreach (var user in users)
    {
        // Debug log
        Console.WriteLine($"🔍 Processing user: Name={user.Name}, Phone={user.Phone}, PAN={user.Pan}, PartnerId='{user.PartnerId}'");

        // Validation: PartnerId is required
        if (string.IsNullOrWhiteSpace(user.PartnerId))
        {
            Console.WriteLine($"❌ Skipping - Missing PartnerId");
            skipped.Add(new { user.Name, user.Phone, user.Pan, reason = "Missing PartnerId" });
            continue;
        }

        // Validation: Either Phone or PAN must be present
        if (string.IsNullOrWhiteSpace(user.Phone) && string.IsNullOrWhiteSpace(user.Pan))
        {
            skipped.Add(new { user.Name, user.PartnerId, reason = "Missing Phone and PAN" });
            continue;
        }

        // Check for duplicates
        bool isDuplicate = false;
        string duplicateReason = "";

        if (!string.IsNullOrWhiteSpace(user.Phone) && existingPhones.Contains(user.Phone))
        {
            isDuplicate = true;
            duplicateReason = "Duplicate phone";
        }

        if (!string.IsNullOrWhiteSpace(user.Pan) && existingPans.Contains(user.Pan))
        {
            isDuplicate = true;
            duplicateReason = duplicateReason == "" ? "Duplicate PAN" : "Duplicate phone and PAN";
        }

        if (isDuplicate)
        {
            skipped.Add(new { user.Name, user.Phone, user.Pan, reason = duplicateReason });
            continue;
        }

        // Add to database context
        db.MoneyViewUsers.Add(user);

        // Add to tracking lists
        if (!string.IsNullOrWhiteSpace(user.Phone))
            existingPhones.Add(user.Phone);
        if (!string.IsNullOrWhiteSpace(user.Pan))
            existingPans.Add(user.Pan);

        inserted.Add(new
        {
            user.Name,
            user.Phone,
            user.Pan,
            user.PartnerId,
            status = "Inserted",
            createdDate = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss")
        });
    }

    // Save all changes at once (more efficient)
    if (inserted.Count > 0)
    {
        try
        {
            await db.SaveChangesAsync();
        }
        catch (Exception ex)
        {
            return Results.Json(new { message = $"Database error: {ex.Message}" }, statusCode: 500);
        }
    }

    // Return appropriate response
    if (inserted.Count > 0 && skipped.Count > 0)
        return Results.Json(new { insertedCount = inserted.Count, skippedCount = skipped.Count, inserted, skipped }, statusCode: 207);

    if (inserted.Count == 0 && skipped.Count > 0)
        return Results.Json(new { skippedCount = skipped.Count, skipped }, statusCode: 409);

    return Results.Json(new { insertedCount = inserted.Count, inserted }, statusCode: 200);
});

app.Run();

// ✅ DbContext + Entity
public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<MoneyViewUser> MoneyViewUsers => Set<MoneyViewUser>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MoneyViewUser>(entity =>
        {
            entity.ToTable("moneyview");
            entity.HasKey(e => e.Id);

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Name).HasColumnName("name");
            entity.Property(e => e.Phone).HasColumnName("phone");
            entity.Property(e => e.Email).HasColumnName("email");
            entity.Property(e => e.Employment).HasColumnName("employment");
            entity.Property(e => e.Pan).HasColumnName("pan");
            entity.Property(e => e.Pincode).HasColumnName("pincode");
            entity.Property(e => e.Income).HasColumnName("income");
            entity.Property(e => e.City).HasColumnName("city");
            entity.Property(e => e.State).HasColumnName("state");
            entity.Property(e => e.Dob).HasColumnName("dob");
            entity.Property(e => e.Gender).HasColumnName("gender");
            entity.Property(e => e.PartnerId).HasColumnName("partner_id").IsRequired();
        });
    }
}

public class MoneyViewUser
{
    public int Id { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("phone")]
    public string? Phone { get; set; }

    [JsonPropertyName("email")]
    public string? Email { get; set; }

    [JsonPropertyName("employment")]
    public string? Employment { get; set; }

    [JsonPropertyName("pan")]
    public string? Pan { get; set; }

    [JsonPropertyName("pincode")]
    public string? Pincode { get; set; }

    [JsonPropertyName("income")]
    public string? Income { get; set; }

    [JsonPropertyName("city")]
    public string? City { get; set; }

    [JsonPropertyName("state")]
    public string? State { get; set; }

    [JsonPropertyName("dob")]
    public string? Dob { get; set; }

    [JsonPropertyName("gender")]
    public string? Gender { get; set; }

    [JsonPropertyName("partnerId")]
    public string PartnerId { get; set; } = string.Empty;
}