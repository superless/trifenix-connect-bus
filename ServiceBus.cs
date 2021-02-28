using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Text;
using System.Threading.Tasks;
using trifenix.connect.input;

namespace trifenix.connect.bus
{
    /// <summary>
    /// Operaciones de bus de servicios
    /// </summary>
    public class ServiceBus {


        private readonly MessageSender Sender;

        /// <summary>
        /// Constructor del servicio de bus
        /// </summary>
        /// <param name="connectionString">cadena de conexión del servicio de bus de Azure</param>
        /// <param name="queueName">nombre de la cola que será utilizada</param>
        public ServiceBus(string connectionString, string queueName) {
            Sender = new MessageSender(connectionString, queueName);
        }

        /// <summary>
        /// Convierte un objeto en bytes para ser serializado
        /// </summary>
        /// <param name="obj">objeto a serializar</param>
        /// <returns></returns>
        public static byte[] Serialize(object obj) {
            string strSerial = JsonConvert.SerializeObject(obj);
            byte[] bytes = Encoding.ASCII.GetBytes(strSerial);
            return bytes;
        }

        /// <summary>
        /// Deserializa un objeto desde un array de bytes
        /// </summary>
        /// <param name="arrBytes">bytes que corresponden a un objeto a ser serializado</param>
        /// <returns>objeto serializado</returns>
        public static JObject Deserialize(byte[] arrBytes) {
            string strSerial = Encoding.ASCII.GetString(arrBytes);
            var obj = (JObject)JsonConvert.DeserializeObject(strSerial);
            return obj;
        }

        private Message GetMessage(object obj, string sessionId) => new Message(Serialize(obj)) { SessionId = sessionId };

        /// <summary>
        /// Pone un elemento en la cola
        /// </summary>
        /// <param name="obj">objeto a poner en la cola</param>
        /// <param name="sessionId">identificador de la sesión de la cola</param>
        /// <returns></returns>
        public async Task PushElement(object obj, string sessionId = null) {
            var message = GetMessage(obj, sessionId);
            await Sender.SendAsync(message);
        }

    }


    /// <summary>
    /// Detalles necesarios para pasar un input a la cola
    /// </summary>
    /// <typeparam name="InputElement"></typeparam>
    public class OperationInstance<InputElement> where InputElement : InputBase {
        public InputElement Element;
        public string Id;
        public Type EntityType;
        public string EntityName;
        public string HttpMethod;
        public string ObjectIdAAD;

        /// <summary>
        /// Constructor de entidad para insertar un input en la cola
        /// </summary>
        /// <param name="_element">objeto a insertar</param>
        /// <param name="_id">identificador opcional del elemento</param>
        /// <param name="_entityName">nombre de la entidad (ej: producto)</param>
        /// <param name="_httpMethod">Método de como se pasará el input</param>
        /// <param name="_objectIdAAD">identificador de active directory</param>
        public OperationInstance(InputElement _element, string _id, string _entityName, string _httpMethod, string _objectIdAAD) {
            Element = _element;
            Id = _id;
            EntityType = _element.GetType();
            EntityName = _entityName;
            HttpMethod = _httpMethod;
            ObjectIdAAD = _objectIdAAD;
        }
    }

}