package com.twitter.finagle.netty4

import io.netty.buffer.ByteBufHolder
import io.netty.channel.{ ChannelHandlerContext, ChannelInboundHandlerAdapter }

// Need to retain the ByteBuf before it's forwarded to a Twitter Future
private[finagle] object ContentRetainer extends ChannelInboundHandlerAdapter {
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit =
    msg match {
      case bb: ByteBufHolder =>
        super.channelRead(ctx, bb.retain())
      case other =>
        super.channelRead(ctx, other)
    }
}