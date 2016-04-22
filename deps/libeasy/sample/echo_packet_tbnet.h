#include <easy_io.h>
#include <byteswap.h>

/**
 * 定义 echo_packet_tbnet_t
 */
typedef struct echo_packet_tbnet_t {
    easy_buf_t              *b;
    char                    *data;
    int                     flag;
    int                     chid;
    int                     pcode;
    int                     datalen;
    char                    buffer[0];
} echo_packet_tbnet_t;

#define ECHO_PACKET_TBNET_HEADER_SIZE 16
#define ECHO_PACKET_TBNET_PACKET_FLAG 0x416e4574
/**
 * decode
 */
void *echo_tbnet_decode(easy_message_t *m)
{
    echo_packet_tbnet_t     *packet;
    int                     flag, chid, pcode, datalen, len;

    if ((len = m->input->last - m->input->pos) < ECHO_PACKET_TBNET_HEADER_SIZE)
        return NULL;

    flag = bswap_32(*((uint32_t *)m->input->pos));
    chid = bswap_32(*((uint32_t *)(m->input->pos + 4)));
    pcode = bswap_32(*((uint32_t *)(m->input->pos + 8)));
    datalen = bswap_32(*((uint32_t *)(m->input->pos + 12)));

    // 检查标记
    if (flag != ECHO_PACKET_TBNET_PACKET_FLAG || datalen < 0 || datalen > 0x4000000) { // 64M
        easy_error_log("flag:%x<>%x, datalen:%d\n", flag, ECHO_PACKET_TBNET_PACKET_FLAG, datalen);
        m->status = EASY_ERROR;
        return NULL;
    }

    // 长度不够
    len -= ECHO_PACKET_TBNET_HEADER_SIZE;

    if (len < datalen) {
        m->next_read_len = datalen - len;
        return NULL;
    }

    // alloc packet
    if ((packet = (echo_packet_tbnet_t *)easy_pool_alloc(m->pool,
                  sizeof(echo_packet_tbnet_t))) == NULL) {
        m->status = EASY_ERROR;
        return NULL;
    }

    m->input->pos += ECHO_PACKET_TBNET_HEADER_SIZE;
    packet->b = NULL;
    packet->chid = chid;
    packet->pcode = pcode;
    packet->datalen = datalen;
    packet->data = (char *)m->input->pos;
    m->input->pos += datalen;

    return packet;
}

/**
 * encode
 */
int echo_tbnet_encode(easy_request_t *r, void *data)
{
    echo_packet_tbnet_t     *packet;
    easy_buf_t              *b;
    int                     len;

    packet = (echo_packet_tbnet_t *) data;

    if ((b = packet->b) == NULL) {
        b = (easy_buf_t *)easy_pool_calloc(r->ms->pool, sizeof(easy_buf_t));
    }

    // set data
    len = packet->datalen + ECHO_PACKET_TBNET_HEADER_SIZE;
    packet->flag = bswap_32(ECHO_PACKET_TBNET_PACKET_FLAG);
    packet->chid = bswap_32(packet->chid);
    packet->pcode = bswap_32(packet->pcode);
    packet->datalen = bswap_32(packet->datalen);

    if (packet->data == &packet->buffer[0]) {
        easy_buf_set_data(r->ms->pool, b, &packet->flag, len);
        easy_request_addbuf(r, b);
    } else {
        len -= ECHO_PACKET_TBNET_HEADER_SIZE;
        easy_buf_set_data(r->ms->pool, b, &packet->flag, ECHO_PACKET_TBNET_HEADER_SIZE);
        easy_request_addbuf(r, b);
        b = easy_buf_pack(r->ms->pool, packet->data, len);
        easy_request_addbuf(r, b);
    }

    return EASY_OK;
}

uint64_t echo_tbnet_packet_id(easy_connection_t *c, void *packet)
{
    return ((echo_packet_tbnet_t *) packet)->chid;
}

echo_packet_tbnet_t *echo_packet_tbnet_rnew(easy_request_t *r, int size)
{
    echo_packet_tbnet_t     *packet;
    easy_buf_t              *b;
    int                     hlen;

    hlen = sizeof(echo_packet_tbnet_t) + sizeof(easy_buf_t);

    if ((b = (easy_buf_t *) easy_pool_alloc(r->ms->pool, size + hlen)) == NULL)
        return NULL;

    packet = (echo_packet_tbnet_t *) (b + 1);
    memset(b, 0, hlen);
    packet->b = b;
    packet->data = &packet->buffer[0];

    return packet;
}

echo_packet_tbnet_t *echo_packet_tbnet_new(easy_session_t **sp, int size)
{
    easy_session_t          *s;
    easy_buf_t              *b;
    echo_packet_tbnet_t     *packet;
    int                     hlen;

    hlen = sizeof(echo_packet_tbnet_t) + sizeof(easy_buf_t);

    if ((s = easy_session_create(size + hlen)) == NULL)
        return NULL;

    b = (easy_buf_t *) & (s->data[0]);
    packet = (echo_packet_tbnet_t *) (b + 1);
    memset(b, 0, hlen);
    b->args = s->pool;

    packet->b = b;
    packet->data = &packet->buffer[0];
    s->r.opacket = packet;
    *sp = s;

    return packet;
}
