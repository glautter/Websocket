using System.Net;
using System.Net.WebSockets;
using System.Text;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseWebSockets();

app.Map("/dpi/api/v1/webhook/{evento}/subscribe", async (HttpContext context, string evento) =>
{
    if (!context.WebSockets.IsWebSocketRequest)
    {
        context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
        return;
    }

    var ws = await context.WebSockets.AcceptWebSocketAsync();
    Console.WriteLine($"[Server] Conexão WebSocket estabelecida para evento: {evento}");

    var buffer = new byte[4096];
    var enableHeartbeat = context.Request.Query.ContainsKey("enableHeartbeat");

    _ = Task.Run(async () =>
    {
        while (ws.State == WebSocketState.Open)
        {
            var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

            if (result.MessageType == WebSocketMessageType.Close)
            {
                Console.WriteLine("[Server] Conexão encerrada pelo cliente.");
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Conexão encerrada", CancellationToken.None);
                return;
            }

            var message = Encoding.UTF8.GetString(buffer, 0, result.Count);
            Console.WriteLine($"[Server] Mensagem recebida: {message}");
        }
    });

    while (ws.State == WebSocketState.Open && enableHeartbeat)
    {
        await Task.Delay(5000);
        var heartbeat = Encoding.UTF8.GetBytes("heartbeat");
        await ws.SendAsync(heartbeat, WebSocketMessageType.Binary, true, CancellationToken.None);
        Console.WriteLine("[Server] Heartbeat enviado");
    }
});

app.Run("http://localhost:5000");
