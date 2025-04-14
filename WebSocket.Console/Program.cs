using System.Net.WebSockets;
using System.Text;

class WebSocketClient
{
    public static async Task Main()
    {
        using var ws = new ClientWebSocket();
        ws.Options.KeepAliveInterval = TimeSpan.FromSeconds(30);
        ws.Options.SetRequestHeader("Authorization", "Bearer <token>");

        var uri = new Uri("ws://localhost:5000/dpi/api/v1/webhook/baixa_callback/subscribe?enableHeartbeat=true");
        await ws.ConnectAsync(uri, CancellationToken.None);
        Console.WriteLine("[Client] Conectado ao WebSocket!");

        var buffer = new byte[4096];
        using var ms = new MemoryStream();

        while (ws.State == WebSocketState.Open)
        {
            ms.Position = 0;
            var payloadSize = 0;
            WebSocketReceiveResult result;

            do
            {
                result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    Console.WriteLine("[Client] Conexão encerrada pelo servidor.");
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, null, CancellationToken.None);
                    return;
                }

                ms.Write(buffer, 0, result.Count);
                payloadSize += result.Count; 

            } while (!result.EndOfMessage);

            var payload = Encoding.UTF8.GetString(ms.ToArray(), 0, payloadSize);
            Console.WriteLine(payload == "heartbeat"
                ? "[Client] Recebido: Heartbeat"
                : $"[Client] Recebido: {payload}");
        }
    }
}
