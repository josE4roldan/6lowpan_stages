/*
 * Copyright (c) 2011, Swedish Institute of Computer Science.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the Institute nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE INSTITUTE AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE INSTITUTE OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * This file is part of the Contiki operating system.
 *
 */

#include "contiki.h"
#include "lib/random.h"
#include "sys/ctimer.h"
#include "sys/etimer.h"
#include "net/ip/uip.h"
#include "net/ipv6/uip-ds6.h"
#include "net/ip/uip-debug.h"

#include "sys/node-id.h"

#include "simple-udp.h"


#include <stdio.h>
#include <string.h>

#define UDP_PORT 1884


#define SEND_INTERVAL		(60 * CLOCK_SECOND)
#define SEND_TIME		(random_rand() % (SEND_INTERVAL))

typedef struct __attribute__((packed)) {
  uint8_t length;
  uint8_t type;
  uint8_t flags;
  uint8_t protocol_id;
  uint16_t duration;
  char client_id[23];
} connect_packet_t;


static struct simple_udp_connection unicast_connection;


static uint16_t keep_alive = 5;
static uint16_t ipv6_broker[] = {0xaaaa, 0, 0, 0, 0, 0, 0, 0x1};
static uip_ipaddr_t broker_addr;
static uint16_t keep_alive_machine = 5;
/*---------------------------------------------------------------------------*/
PROCESS(unicast_sender_process, "Unicast sender example process");
AUTOSTART_PROCESSES(&unicast_sender_process);
/*---------------------------------------------------------------------------*/
static void
receiver(struct simple_udp_connection *c,
         const uip_ipaddr_t *sender_addr,
         uint16_t sender_port,
         const uip_ipaddr_t *receiver_addr,
         uint16_t receiver_port,
         const uint8_t *data,
         uint16_t datalen)
{
  printf("Data received on port %d from port %d with length %d\n",
         receiver_port, sender_port, datalen);
}
/*---------------------------------------------------------------------------*/


static void
set_global_address(void)
{
  uip_ipaddr_t ipaddr;
  int i;
  uint8_t state;

  uip_ip6addr(&ipaddr, UIP_DS6_DEFAULT_PREFIX, 0, 0, 0, 0, 0, 0, 0);
  uip_ds6_set_addr_iid(&ipaddr, &uip_lladdr);
  uip_ds6_addr_add(&ipaddr, 0, ADDR_AUTOCONF);

  printf("IPv6 addresses: ");
  for(i = 0; i < UIP_DS6_ADDR_NB; i++) {
    state = uip_ds6_if.addr_list[i].state;
    if(uip_ds6_if.addr_list[i].isused &&
       (state == ADDR_TENTATIVE || state == ADDR_PREFERRED)) {
      uip_debug_ipaddr_print(&uip_ds6_if.addr_list[i].ipaddr);
      printf("\n");
    }
  }
}
/*---------------------------------------------------------------------------*/
PROCESS_THREAD(unicast_sender_process, ev, data)
{
  static struct etimer periodic_timer;
  static struct etimer send_timer;
  uint8_t buff[]={0x16,0x04,0x04,0x01,0x00,0x05,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x30,0x31};


  PROCESS_BEGIN();

  uip_ip6addr(&broker_addr, ipv6_broker[0],
                          ipv6_broker[1],
                          ipv6_broker[2],
                          ipv6_broker[3],
                          ipv6_broker[4],
                          ipv6_broker[5],
                          ipv6_broker[6],
                          ipv6_broker[7]);

  //set_global_address();
  connect_packet_t packet;
  packet.flags = (0x1 << 2);
  packet.protocol_id = 0x01;
  packet.type = 0x04;
  packet.duration =  uip_htons(keep_alive_machine);
  char prueba[] = {0x25,0x33};

  sprintf(packet.client_id,"00000000000000001");

  packet.client_id[strlen(packet.client_id)] = '\0';
  packet.length = 0x06 + strlen(packet.client_id);

  simple_udp_register(&unicast_connection, UDP_PORT,
                      NULL, UDP_PORT, receiver);

  etimer_set(&periodic_timer, SEND_INTERVAL);
  simple_udp_sendto(&unicast_connection, &packet, packet.length , &broker_addr);
  while(1) {

    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));
    etimer_reset(&periodic_timer);
    etimer_set(&send_timer, SEND_TIME);

    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&send_timer));

    if(&broker_addr != NULL) {
      static unsigned int message_number;


      printf("Sending unicast to ");
      uip_debug_ipaddr_print(&broker_addr);
      printf("\n");
      //sprintf(buf, "Message %d", message_number);

      message_number++;
      simple_udp_sendto(&unicast_connection, buff,sizeof(buff) , &broker_addr);
      
    } else {
      printf("Service  not found\n");
    }
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/
