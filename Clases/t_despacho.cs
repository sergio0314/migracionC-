using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_despacho
    {
        public string IdSucursal { get; set; }
        public string IdDocumento { get; set; }
        public string Consecutivo { get; set; }
        public int Numero { get; set; }
        public string IdProducto { get; set; }
        public decimal CantidadReg { get; set; }
        public decimal CantidadEnt { get; set; }
        public DateTime Fecha { get; set; }
        public long? IdTercero { get; set; }
        public int Despacho { get; set; }
        public string TipoProceso { get; set; }
        public string  Observacion { get; set; }
        public string DirecEntrega { get; set; }
        public string TelefEntrega { get; set; }
        public string IdSucursalTras { get; set; }
        public string DocumentoTras { get; set; }
        public string ConsecutivoTras { get; set; }
        public int NumeroTras { get; set; }
        public DateTime FechaDespacho { get; set; }
        public string IdUsuarioDespacho { get; set; }
        public int Estado { get; set; }
        public DateTime FechaEn { get; set; }
        public int Estados { get; set; }
        public int IdJornada { get; set; }
        public int CodigoAut { get; set; }
        public int IdDomicilio { get; set; }
        public string TipoVenta { get; set; }
        public int Impreso { get; set; }
        public string Referencia { get; set; }
        public int Entrega { get; set; }
        public string Observacion2 { get; set; }
        public DateTime FechaEntrega { get; set; }
        public string IdZona { get; set; }
        public string IdSucursalDes { get; set; }
        public int Pendiente { get; set; }
        public string IdDocumentoSale { get; set; }
        public string IdBodegaDesp { get; set; }
    }
}
