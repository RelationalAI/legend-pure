// Copyright 2022 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.pure.m3.compiler.postprocessing.observer;

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.block.factory.HashingStrategies;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMapWithHashingStrategy;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;

class SimpleTimingPostProcessorObserver extends TimingPostProcessorObserver
{
    private final MutableObjectLongMap<CoreInstance> starts = ObjectLongHashMapWithHashingStrategy.newMap(HashingStrategies.identityStrategy());

    SimpleTimingPostProcessorObserver()
    {
    }

    @Override
    protected void noteProcessingStart(CoreInstance instance, long startNanoTime)
    {
        this.starts.put(instance, startNanoTime);
    }

    @Override
    protected long noteProcessingEnd(CoreInstance instance, long endNanoTime)
    {
        long startNanoTime = this.starts.removeKeyIfAbsent(instance, endNanoTime);
        return endNanoTime - startNanoTime;
    }
}
