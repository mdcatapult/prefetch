/*
 * Copyright 2024 Medicines Discovery Catapult
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mdcatapult.doclib.handlers

import io.mdcatapult.doclib.consumer.HandlerResult
import io.mdcatapult.doclib.models.DoclibDoc

/**
 * This is a convenience class to avoid typing difficulties with the Queue class. It expects
 * a response type but if you are just sending to a queue you don't actually care.
 * @param doclibDoc
 */
case class SupervisorHandlerResult(doclibDoc: DoclibDoc) extends HandlerResult
